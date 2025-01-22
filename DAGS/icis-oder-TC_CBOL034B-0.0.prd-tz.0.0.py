from datetime import datetime, timedelta
from kubernetes.client import models as k8s
from airflow.providers.http.operators.http import SimpleHttpOperator
from airflow.operators.dummy import DummyOperator
from airflow.operators.python_operator import PythonOperator
from airflow.operators.python import BranchPythonOperator
from airflow.operators.bash import BashOperator
from airflow.operators.empty import EmptyOperator
from airflow.operators.subdag import SubDagOperator
from airflow.utils.trigger_rule import TriggerRule
from airflow.utils.helpers import chain, cross_downstream
from airflow.providers.cncf.kubernetes.operators.kubernetes_pod import (
    KubernetesPodOperator,
)
import pendulum
local_tz = pendulum.timezone("Asia/Seoul")
import sys
sys.path.append('/opt/bitnami/airflow/dags/git_sa-common')

from icis_common import *
COMMON = ICISCmmn(DOMAIN='oder',ENV='prd-tz', NAMESPACE='t-order'
                , WORKFLOW_NAME='TC_CBOL034B',WORKFLOW_ID='75ef02720daf436084f7fd67278b4122', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-TC_CBOL034B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 26, 18, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('75ef02720daf436084f7fd67278b4122')

    cbol034bJob_vol = []
    cbol034bJob_volMnt = []
    cbol034bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol034bJob_env.extend([getICISConfigMap('icis-oder-trmncust-batch-mng-configmap'), getICISSecret('icis-oder-trmncust-batch-mng-secret'), getICISConfigMap('icis-oder-trmncust-batch-configmap'), getICISSecret('icis-oder-trmncust-batch-secret')])
    cbol034bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol034bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '378a337b94d648a38fddf07bc16d16a3',
        'volumes': cbol034bJob_vol,
        'volume_mounts': cbol034bJob_volMnt,
        'env_from':cbol034bJob_env,
        'task_id':'cbol034bJob',
        'image':'/icis/icis-oder-trmncust-batch:0.7.1.1',
        'arguments':["--job.name=cbol034bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('75ef02720daf436084f7fd67278b4122')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol034bJob,
        Complete
    ]) 

    # authCheck >> cbol034bJob >> Complete
    workflow









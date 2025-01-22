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
                , WORKFLOW_NAME='TC_CBOL035B',WORKFLOW_ID='e9f29e7b40db46f38738244e8560d1a3', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-TC_CBOL035B-0.0.prd-tz.0.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 7, 26, 18, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('e9f29e7b40db46f38738244e8560d1a3')

    cbol035bJob_vol = []
    cbol035bJob_volMnt = []
    cbol035bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol035bJob_env.extend([getICISConfigMap('icis-oder-trmncust-batch-mng-configmap'), getICISSecret('icis-oder-trmncust-batch-mng-secret'), getICISConfigMap('icis-oder-trmncust-batch-configmap'), getICISSecret('icis-oder-trmncust-batch-secret')])
    cbol035bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol035bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '5b8467f4693e411bbac4ad033ab0c5eb',
        'volumes': cbol035bJob_vol,
        'volume_mounts': cbol035bJob_volMnt,
        'env_from':cbol035bJob_env,
        'task_id':'cbol035bJob',
        'image':'/icis/icis-oder-trmncust-batch:0.7.1.1',
        'arguments':["--job.name=cbol035bJob", "requestDate="+str(datetime.now())],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('e9f29e7b40db46f38738244e8560d1a3')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol035bJob,
        Complete
    ]) 

    # authCheck >> cbol035bJob >> Complete
    workflow









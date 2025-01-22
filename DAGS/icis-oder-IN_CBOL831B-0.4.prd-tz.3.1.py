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
                , WORKFLOW_NAME='IN_CBOL831B',WORKFLOW_ID='94a950e1bb98493e998d99ed3c0cb1dd', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-IN_CBOL831B-0.4.prd-tz.3.1'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2025, 1, 1, 0, 0, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
    ,'max_active_runs': 16
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('94a950e1bb98493e998d99ed3c0cb1dd')

    cbol831bJob_vol = []
    cbol831bJob_volMnt = []
    cbol831bJob_env = [getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret'), getICISConfigMap('icis-oder-configmap'), getICISSecret('icis-oder-secret')]
    cbol831bJob_env.extend([getICISConfigMap('icis-oder-intelnet-batch-mng-configmap'), getICISSecret('icis-oder-intelnet-batch-mng-secret'), getICISConfigMap('icis-oder-intelnet-batch-configmap'), getICISSecret('icis-oder-intelnet-batch-secret')])
    cbol831bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol831bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : '89b787912f9945fb8158f2171ca60568',
        'volumes': cbol831bJob_vol,
        'volume_mounts': cbol831bJob_volMnt,
        'env_from':cbol831bJob_env,
        'task_id':'cbol831bJob',
        'image':'/icis/icis-oder-intelnet-batch:0.7.1.7',
        'arguments':["--job.name=cbol831bJob",   "date=${YYYYMMDDHHMISS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('94a950e1bb98493e998d99ed3c0cb1dd')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol831bJob,
        Complete
    ]) 

    # authCheck >> cbol831bJob >> Complete
    workflow









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
                , WORKFLOW_NAME='WC_CBOL210B',WORKFLOW_ID='09dcd2650bba4010b4335ab1c8c188e8', APP_NAME='NBSS_TORD', CHNL_TYPE='TO', USER_ID='91337909')

with COMMON.getICISDAG({
    'dag_id':'icis-oder-WC_CBOL210B-0.0.prd-tz.4.0'
    ,'schedule_interval':'@once'
    ,'start_date': datetime(2024, 11, 8, 10, 41, 00, tzinfo=local_tz)
    ,'end_date': None
    ,'paused': False
})as dag:

    authCheck = COMMON.getICISAuthCheckWflow('09dcd2650bba4010b4335ab1c8c188e8')

    cbol210bJob_vol = []
    cbol210bJob_volMnt = []
    cbol210bJob_vol.append(getVolume('shared-volume','shared-volume'))
    cbol210bJob_volMnt.append(getVolumeMount('shared-volume','/app/order'))

    cbol210bJob_env = [getICISConfigMap('icis-oder-wrlincomn-batch-configmap'), getICISConfigMap('icis-oder-wrlincomn-batch-configmap2'), getICISSecret('icis-oder-wrlincomn-batch-secret')]
    cbol210bJob_env.extend([getICISConfigMap('icis-oder-cmmn-configmap'), getICISSecret('icis-oder-cmmn-secret')])
    cbol210bJob_env.extend([getICISConfigMap('icis-oder-truststore.jks')])

    cbol210bJob = COMMON.getICISKubernetesPodOperator_v1({
        'id' : 'abf70baf010e4a0b9360da923ccda41f',
        'volumes': cbol210bJob_vol,
        'volume_mounts': cbol210bJob_volMnt,
        'env_from':cbol210bJob_env,
        'task_id':'cbol210bJob',
        'image':'/icis/icis-oder-wrlincomn-batch:0.7.1.2',
        'arguments':["--job.name=cbol210bJob", "pgmNm=cbol210b", "workDate=20240731", "requestDate=${YYYYMMDDHHMISSSSS}"],
        'taskAlrmStYn': 'N', # 시작 알림 전송
        'taskAlrmFnsYn': 'N' # 종료 알림 전송
    })

      
       
      

    Complete = COMMON.getICISCompleteWflowTask('09dcd2650bba4010b4335ab1c8c188e8')

    workflow = COMMON.getICISPipeline([
        authCheck,
        cbol210bJob,
        Complete
    ]) 

    # authCheck >> cbol210bJob >> Complete
    workflow








